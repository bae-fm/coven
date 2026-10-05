//! Every opened key remains available by its store or circle key number (§11).

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;

use subtle::ConstantTimeEq;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{cipher, derivation, randomness, wire};
use crate::{
    CircleId, CryptoError, DerivedKeys, EncryptionKey, MaterialError, SealError, SecretBytes,
};

/// The store's `n`th 32-byte key (§11), erased on drop.
/// Cloning supplies an independent unlocked snapshot to in-memory custody.
#[derive(Clone)]
pub struct StoreKey {
    number: NonZeroU64,
    bytes: Zeroizing<[u8; 32]>,
}

impl StoreKey {
    /// Make the store's `number`th key using the operating system's randomness.
    pub fn generate(number: u64) -> Result<Self, CryptoError> {
        let number = NonZeroU64::new(number).ok_or(MaterialError::ZeroKeyNumber)?;
        let mut bytes = Zeroizing::new([0; 32]);
        randomness::fill(bytes.as_mut())?;
        Ok(Self { number, bytes })
    }

    /// Import a store key opened from custody or a member's sealed copy (§11).
    pub fn from_bytes(number: u64, bytes: [u8; 32]) -> Result<Self, MaterialError> {
        let bytes = Zeroizing::new(bytes);
        let number = NonZeroU64::new(number).ok_or(MaterialError::ZeroKeyNumber)?;
        Ok(Self { number, bytes })
    }

    /// Which store key this is, starting at one (§11).
    pub fn number(&self) -> u64 {
        self.number.get()
    }

    /// Derive separate encryption, naming, file-nonce and fingerprint keys.
    pub fn derive(&self) -> Result<DerivedKeys, CryptoError> {
        DerivedKeys::new(&self.bytes)
    }

    pub(crate) fn encode_into(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.number().to_le_bytes());
        bytes.extend_from_slice(self.bytes.as_ref());
    }

    pub(crate) fn decode(bytes: &mut &[u8]) -> Result<Self, MaterialError> {
        Self::from_bytes(wire::number(bytes)?, wire::array(bytes)?)
    }
}

impl fmt::Debug for StoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreKey")
            .field("number", &self.number)
            .finish_non_exhaustive()
    }
}

/// One circle's numbered 32-byte key, replaced when someone leaves (§14.3).
/// Cloning supplies an independent unlocked snapshot to in-memory custody.
#[derive(Clone, Debug)]
pub struct CircleKey {
    circle: CircleId,
    key: StoreKey,
}

impl CircleKey {
    /// Make a circle key using the operating system's randomness.
    pub fn generate(circle: CircleId, number: u64) -> Result<Self, CryptoError> {
        Ok(Self {
            circle,
            key: StoreKey::generate(number)?,
        })
    }

    /// Import a circle key opened from custody or a member's sealed copy.
    pub fn from_bytes(
        circle: CircleId,
        number: u64,
        bytes: [u8; 32],
    ) -> Result<Self, MaterialError> {
        Ok(Self {
            circle,
            key: StoreKey::from_bytes(number, bytes)?,
        })
    }

    /// The circle whose rows and files this key protects.
    pub fn circle(&self) -> CircleId {
        self.circle
    }

    /// Which key of this circle this is, starting at one.
    pub fn number(&self) -> u64 {
        self.key.number()
    }

    /// Derive separate encryption, naming, file-nonce and fingerprint keys.
    pub fn derive(&self) -> Result<DerivedKeys, CryptoError> {
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
    stores: BTreeMap<u64, StoreKey>,
    circles: BTreeMap<(CircleId, u64), CircleKey>,
}

impl StoreKeyring {
    /// Start with one opened store key. A keyring always holds a store key.
    pub fn new(key: StoreKey) -> Self {
        Self {
            stores: BTreeMap::from([(key.number(), key)]),
            circles: BTreeMap::new(),
        }
    }

    /// Keep an opened store key; a different key at the same number is an error.
    pub fn insert_store_key(&mut self, key: StoreKey) -> Result<(), MaterialError> {
        if let Some(existing) = self.stores.get(&key.number()) {
            if !bool::from(existing.bytes.as_ref().ct_eq(key.bytes.as_ref())) {
                return Err(MaterialError::StoreKeyConflict(key.number()));
            }
        } else {
            self.stores.insert(key.number(), key);
        }
        Ok(())
    }

    /// Keep an opened circle key, retaining its earlier keys too (§14.3).
    pub fn insert_circle_key(&mut self, key: CircleKey) -> Result<(), MaterialError> {
        let id = (key.circle(), key.number());
        if let Some(existing) = self.circles.get(&id) {
            if !bool::from(existing.key.bytes.as_ref().ct_eq(key.key.bytes.as_ref())) {
                return Err(MaterialError::CircleKeyConflict {
                    circle: id.0,
                    number: id.1,
                });
            }
        } else {
            self.circles.insert(id, key);
        }
        Ok(())
    }

    /// The highest numbered store key, used for new writes and app data (§11).
    pub fn current_store_key(&self) -> Result<&StoreKey, MaterialError> {
        self.stores
            .last_key_value()
            .map(|(_, key)| key)
            .ok_or(MaterialError::EmptyKeyring)
    }

    /// An opened store key by its number, so older writes still open (§11).
    pub fn store_key(&self, number: u64) -> Result<&StoreKey, MaterialError> {
        self.stores
            .get(&number)
            .ok_or(MaterialError::UnknownStoreKey(number))
    }

    /// An opened circle key by its circle and number (§14.3).
    pub fn circle_key(&self, circle: CircleId, number: u64) -> Result<&CircleKey, MaterialError> {
        self.circles
            .get(&(circle, number))
            .ok_or(MaterialError::UnknownCircleKey { circle, number })
    }

    /// The highest numbered opened key of a circle, for its new rows and files.
    pub fn current_circle_key(&self, circle: CircleId) -> Option<&CircleKey> {
        self.circles
            .range((circle, 1)..=(circle, u64::MAX))
            .next_back()
            .map(|(_, key)| key)
    }

    /// Encode every opened key for custody. The returned bytes remain secret.
    pub fn to_secret_bytes(&self) -> SecretBytes {
        // Allocate once: growing a secret Vec would abandon an unerased copy.
        let mut bytes = Zeroizing::new(Vec::with_capacity(
            21 + self.stores.len() * 40 + self.circles.len() * 56,
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
        if count == 0 {
            return Err(MaterialError::EmptyKeyring);
        }
        if count > (bytes.len() / 40) as u64 {
            return Err(MaterialError::Encoding);
        }
        let mut stores = BTreeMap::new();
        for _ in 0..count {
            let key = StoreKey::decode(&mut bytes)?;
            if stores.insert(key.number(), key).is_some() {
                return Err(MaterialError::Encoding);
            }
        }
        let count = wire::number(&mut bytes)?;
        if count > (bytes.len() / 56) as u64 {
            return Err(MaterialError::Encoding);
        }
        let mut circles = BTreeMap::new();
        for _ in 0..count {
            let key = CircleKey::decode(&mut bytes)?;
            if circles.insert((key.circle(), key.number()), key).is_some() {
                return Err(MaterialError::Encoding);
            }
        }
        wire::end(bytes)?;
        Ok(Self { stores, circles })
    }

    /// Encrypt app data with its own key derived from the current store key (§20.11).
    /// The app's associated data binds the value to its place.
    /// The authenticated header records the store key number for later opening.
    pub fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        let key = self.current_store_key()?;
        let mut header = b"CVAD\x01".to_vec();
        header.extend_from_slice(&key.number().to_le_bytes());
        let context = cipher::context(&[&header, aad]);
        let encryption = derivation::derive(&key.bytes, derivation::APP_DATA)?;
        let body = cipher::seal_random(&encryption, &context, plaintext)?;
        header.extend(body);
        Ok(header)
    }

    /// Open app data with the store key it names, even after replacement (§20.11).
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        let mut bytes = sealed;
        wire::prefix(&mut bytes, b"CVAD\x01")?;
        let number = wire::number(&mut bytes)?;
        let header = sealed.get(..13).ok_or(MaterialError::Encoding)?;
        let key = self.store_key(number)?;
        let encryption = derivation::derive(&key.bytes, derivation::APP_DATA)?;
        Ok(cipher::open_random(
            &encryption,
            &cipher::context(&[header, aad]),
            bytes,
        )?)
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
    pub fn join_request_key(&self) -> Result<EncryptionKey, CryptoError> {
        Ok(EncryptionKey(derivation::derive(
            &self.0,
            derivation::JOIN_REQUEST,
        )?))
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
