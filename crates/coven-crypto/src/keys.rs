//! Every opened key remains available by its store or circle key id (§11).

use std::collections::BTreeMap;
use std::fmt;

use coven_foundation::id_source::KeyId;
use subtle::ConstantTimeEq;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{derivation, randomness, wire};
use crate::{CircleId, CryptoError, DerivedKeys, EncryptionKey, MaterialError, SecretBytes};

/// A store's identified 32-byte key (§11), erased on drop.
/// Cloning supplies an independent unlocked snapshot to in-memory custody.
#[derive(Clone)]
pub struct StoreKey {
    id: KeyId,
    bytes: Zeroizing<[u8; 32]>,
}

impl StoreKey {
    /// Make random key material for an identity supplied by the id source.
    pub fn generate(id: KeyId) -> Result<Self, CryptoError> {
        let mut bytes = Zeroizing::new([0; 32]);
        randomness::fill(bytes.as_mut())?;
        Ok(Self { id, bytes })
    }

    /// Import a store key opened from custody or a member's sealed copy (§11).
    pub fn from_bytes(id: KeyId, bytes: [u8; 32]) -> Self {
        let bytes = Zeroizing::new(bytes);
        Self { id, bytes }
    }

    /// The random identity named by the entry introducing this key (§11).
    pub fn id(&self) -> KeyId {
        self.id
    }

    /// Derive separate encryption and fingerprint keys.
    pub fn derive(&self) -> DerivedKeys {
        DerivedKeys::new(&self.bytes)
    }

    pub(crate) fn encode_into(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(self.id().0.as_bytes());
        bytes.extend_from_slice(self.bytes.as_ref());
    }

    pub(crate) fn decode(bytes: &mut &[u8]) -> Result<Self, MaterialError> {
        let id = KeyId(Uuid::from_bytes(wire::array(bytes)?));
        Ok(Self::from_bytes(id, wire::array(bytes)?))
    }
}

impl fmt::Debug for StoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreKey")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// One circle's identified 32-byte key, replaced when someone leaves (§14.3).
/// Cloning supplies an independent unlocked snapshot to in-memory custody.
#[derive(Clone, Debug)]
pub struct CircleKey {
    circle: CircleId,
    key: StoreKey,
}

impl CircleKey {
    /// Make a circle key using the operating system's randomness.
    pub fn generate(circle: CircleId, id: KeyId) -> Result<Self, CryptoError> {
        Ok(Self {
            circle,
            key: StoreKey::generate(id)?,
        })
    }

    /// Import a circle key opened from custody or a member's sealed copy.
    pub fn from_bytes(circle: CircleId, id: KeyId, bytes: [u8; 32]) -> Self {
        Self {
            circle,
            key: StoreKey::from_bytes(id, bytes),
        }
    }

    /// The circle whose rows and files this key protects.
    pub fn circle(&self) -> CircleId {
        self.circle
    }

    /// The random identity named by the entry introducing this key.
    pub fn id(&self) -> KeyId {
        self.key.id()
    }

    /// Derive separate encryption and fingerprint keys.
    pub fn derive(&self) -> DerivedKeys {
        self.key.derive()
    }

    pub(crate) fn encode_into(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(self.circle.0.as_bytes());
        self.key.encode_into(bytes);
    }

    pub(crate) fn decode(bytes: &mut &[u8]) -> Result<Self, MaterialError> {
        Ok(Self {
            circle: CircleId(Uuid::from_bytes(wire::array(bytes)?)),
            key: StoreKey::decode(bytes)?,
        })
    }
}

/// Every store and circle key this device has opened, including older keys (§11).
/// Cloning lets in-memory custody keep its copy while returning an unlocked one.
#[derive(Clone, Debug)]
pub struct StoreKeyring {
    stores: BTreeMap<KeyId, StoreKey>,
    circles: BTreeMap<(CircleId, KeyId), CircleKey>,
}

impl StoreKeyring {
    /// Start with one opened store key. A keyring always holds a store key.
    pub fn new(key: StoreKey) -> Self {
        Self {
            stores: BTreeMap::from([(key.id(), key)]),
            circles: BTreeMap::new(),
        }
    }

    /// Keep an opened store key; a different key at the same id is an error.
    pub fn insert_store_key(&mut self, key: StoreKey) -> Result<(), MaterialError> {
        if let Some(existing) = self.stores.get(&key.id()) {
            if !bool::from(existing.bytes.as_ref().ct_eq(key.bytes.as_ref())) {
                return Err(MaterialError::StoreKeyConflict(key.id()));
            }
        } else {
            self.stores.insert(key.id(), key);
        }
        Ok(())
    }

    /// Keep an opened circle key, retaining its earlier keys too (§14.3).
    pub fn insert_circle_key(&mut self, key: CircleKey) -> Result<(), MaterialError> {
        let id = (key.circle(), key.id());
        if let Some(existing) = self.circles.get(&id) {
            if !bool::from(existing.key.bytes.as_ref().ct_eq(key.key.bytes.as_ref())) {
                return Err(MaterialError::CircleKeyConflict {
                    circle: id.0,
                    key: id.1,
                });
            }
        } else {
            self.circles.insert(id, key);
        }
        Ok(())
    }

    /// An opened store key by identity, so older writes still open (§11).
    pub fn store_key(&self, key: KeyId) -> Result<&StoreKey, MaterialError> {
        self.stores
            .get(&key)
            .ok_or(MaterialError::UnknownStoreKey(key))
    }

    /// All held store-key identities, including keys preceding a member's join.
    pub fn store_key_ids(&self) -> impl Iterator<Item = KeyId> + '_ {
        self.stores.keys().copied()
    }

    /// All held keys for a circle, including its earlier membership epochs.
    pub fn circle_key_ids(&self, circle: CircleId) -> impl Iterator<Item = KeyId> + '_ {
        self.circles
            .keys()
            .filter_map(move |(owner, key)| (*owner == circle).then_some(*key))
    }

    /// An opened circle key by its circle and identity (§14.3).
    pub fn circle_key(&self, circle: CircleId, key: KeyId) -> Result<&CircleKey, MaterialError> {
        self.circles
            .get(&(circle, key))
            .ok_or(MaterialError::UnknownCircleKey { circle, key })
    }

    /// Encode every opened key for custody. The returned bytes remain secret.
    pub fn to_secret_bytes(&self) -> SecretBytes {
        // Allocate once: growing a secret Vec would abandon an unerased copy.
        let mut bytes = Zeroizing::new(Vec::with_capacity(
            21 + self.stores.len() * 48 + self.circles.len() * 64,
        ));
        bytes.extend_from_slice(b"CVKR\x01");
        bytes.extend_from_slice(&(self.stores.len() as u64).to_le_bytes());
        for key in self.stores.values() {
            key.encode_into(&mut bytes);
        }
        bytes.extend_from_slice(&(self.circles.len() as u64).to_le_bytes());
        for key in self.circles.values() {
            key.encode_into(&mut bytes);
        }
        SecretBytes(bytes)
    }

    /// Decode custody bytes, rejecting malformed, duplicate or empty keyrings.
    pub fn from_secret_bytes(mut bytes: &[u8]) -> Result<Self, MaterialError> {
        wire::prefix(&mut bytes, b"CVKR\x01")?;
        let count = wire::number(&mut bytes)?;
        if count == 0 || count > (bytes.len() / 48) as u64 {
            return Err(MaterialError::Encoding);
        }
        let mut stores = BTreeMap::new();
        for _ in 0..count {
            let key = StoreKey::decode(&mut bytes)?;
            if stores.insert(key.id(), key).is_some() {
                return Err(MaterialError::Encoding);
            }
        }
        let count = wire::number(&mut bytes)?;
        if count > (bytes.len() / 64) as u64 {
            return Err(MaterialError::Encoding);
        }
        let mut circles = BTreeMap::new();
        for _ in 0..count {
            let key = CircleKey::decode(&mut bytes)?;
            if circles.insert((key.circle(), key.id()), key).is_some() {
                return Err(MaterialError::Encoding);
            }
        }
        wire::end(bytes)?;
        Ok(Self { stores, circles })
    }
}

/// The one-time invite secret lets a person ask to join, not open the store (§12.2).
pub struct InviteSecret(Zeroizing<[u8; 32]>);

impl InviteSecret {
    /// Make an invite secret using the operating system's randomness.
    pub fn generate() -> Result<Self, CryptoError> {
        let mut bytes = Zeroizing::new([0; 32]);
        randomness::fill(bytes.as_mut())?;
        Ok(Self(bytes))
    }

    /// Import the secret carried by a scanned invite (§12.2).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Encode the secret for the invite code, keeping its allocation zeroizing.
    pub fn to_secret_bytes(&self) -> SecretBytes {
        SecretBytes::new(self.0.to_vec())
    }

    /// Derive the join request's encryption key with its own HKDF label (§11.1).
    pub fn join_request_key(&self) -> EncryptionKey {
        EncryptionKey(derivation::derive_label(&self.0, derivation::JOIN_REQUEST))
    }
}

impl fmt::Debug for InviteSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InviteSecret([REDACTED])")
    }
}

#[cfg(test)]
#[path = "keys_tests.rs"]
mod tests;
