//! A member's signing and sealing key pairs, kept together (§11.1).

use crate::{cipher, derivation, randomness, wire};
use crate::{CircleKey, CryptoError, MaterialError, ObjectDigest, SecretBytes, StoreKey};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use std::{fmt, str::FromStr};
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

/// A member, by the public half of their Ed25519 key pair (Appendix E).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemberId([u8; 32]);

impl MemberId {
    /// Validate a member's public key, refusing weak Ed25519 points.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, CryptoError> {
        let key = VerifyingKey::from_bytes(&bytes).map_err(|_| CryptoError::InvalidMemberId)?;
        if key.is_weak() {
            return Err(CryptoError::InvalidMemberId);
        }
        Ok(Self(bytes))
    }

    /// The validated Ed25519 public key's 32 bytes.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Verify a detached signature against this member's public key (§10).
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), CryptoError> {
        let key = VerifyingKey::from_bytes(&self.0)
            .expect("MemberId contains a validated Ed25519 public key");
        key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(&signature.0))
            .map_err(|_| CryptoError::Signature)
    }

    /// Verify an object's signature after hashing all bytes before it (§14.4).
    /// The domain and path are authenticated along with the SHA-256 digest.
    /// Panics if the storage path is empty.
    pub fn verify_object(
        &self,
        path: &str,
        digest: &ObjectDigest,
        signature: &Signature,
    ) -> Result<(), CryptoError> {
        self.verify(&object_message(path, digest), signature)
    }
}

impl FromStr for MemberId {
    type Err = CryptoError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || value
                .bytes()
                .any(|b| !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b))
        {
            return Err(CryptoError::InvalidMemberId);
        }
        let mut bytes = [0; 32];
        hex::decode_to_slice(value, &mut bytes).map_err(|_| CryptoError::InvalidMemberId)?;
        Self::from_bytes(bytes)
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

/// The public half of a member's X25519 pair, used for anonymous sealed keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealingPublicKey([u8; 32]);

impl SealingPublicKey {
    /// Read a public sealing key. Low-order keys are refused when sealing.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// The public bytes carried with the member's join request (§12.2).
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A detached Ed25519 signature on bytes authored by a member (§10).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Signature([u8; 64]);

impl Signature {
    /// Read a detached signature; authenticity is checked by `MemberId::verify`.
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
    /// The signature bytes written beside the signed message.
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

/// The kind-37 envelope of a store or circle key, retaining its random bytes.
/// Decoding checks framing only; [`MemberKeys`] authenticates and opens it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedKey<'a> {
    ephemeral: [u8; 32],
    body: &'a [u8],
}

impl<'a> SealedKey<'a> {
    /// Decode exactly one envelope, checking its fixed lengths before allocation.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, CryptoError> {
        let prefix = bytes.get(..3).ok_or(CryptoError::Malformed)?;
        if prefix[0] != 37 {
            return Err(CryptoError::UnknownKind(prefix[0]));
        }
        let version = u16::from_be_bytes([prefix[1], prefix[2]]);
        if version != 1 {
            return Err(CryptoError::UnsupportedVersion(version));
        }
        if !matches!(bytes.len(), 123 | 139) {
            return Err(CryptoError::Malformed);
        }
        Ok(Self {
            ephemeral: bytes[3..35].try_into().expect("checked envelope length"),
            body: &bytes[35..],
        })
    }

    /// Encode the same ephemeral key, nonce, ciphertext and tag without resealing.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(35 + self.body.len());
        bytes.extend_from_slice(&[37, 0, 1]);
        bytes.extend_from_slice(&self.ephemeral);
        bytes.extend_from_slice(self.body);
        bytes
    }
}

/// A member's Ed25519 signing pair and independent X25519 sealing pair (§11.1).
/// Both secrets erase on drop. Clone is needed by in-memory custody to keep
/// its keys while returning an independently owned unlocked pair.
#[derive(Clone)]
pub struct MemberKeys {
    signing: SigningKey,
    sealing: StaticSecret,
}

impl MemberKeys {
    /// Make both member key pairs using the operating system's randomness.
    pub fn generate() -> Result<Self, CryptoError> {
        let mut signing = Zeroizing::new([0; 32]);
        let mut sealing = Zeroizing::new([0; 32]);
        randomness::fill(signing.as_mut())?;
        randomness::fill(sealing.as_mut())?;
        Ok(Self {
            signing: SigningKey::from_bytes(&signing),
            sealing: StaticSecret::from(*sealing),
        })
    }

    /// This member, identified by their public Ed25519 key (Appendix E).
    pub fn member_id(&self) -> MemberId {
        MemberId(self.signing.verifying_key().to_bytes())
    }

    /// The public X25519 key to which store and circle keys are sealed (§11).
    pub fn sealing_public_key(&self) -> SealingPublicKey {
        SealingPublicKey(PublicKey::from(&self.sealing).to_bytes())
    }

    /// Sign bytes with the member's Ed25519 pair (§10).
    pub fn sign(&self, bytes: &[u8]) -> Signature {
        Signature(self.signing.sign(bytes).to_bytes())
    }

    /// Sign the digest of every object byte before the signature (§14.4).
    /// Feed the prefix, length fields and sealed chunks to [`crate::ObjectHasher`]
    /// in storage order. The signed message binds its domain, path and digest.
    /// Panics if the storage path is empty.
    ///
    /// A file's content hash cannot stand in for an object's digest.
    /// ```compile_fail
    /// use coven_crypto::{ContentHasher, MemberKeys};
    /// let member = MemberKeys::generate().unwrap();
    /// member.sign_object("devices/1/1", &ContentHasher::new().finish());
    /// ```
    pub fn sign_object(&self, path: &str, digest: &ObjectDigest) -> Signature {
        self.sign(&object_message(path, digest))
    }

    /// Encode both private seeds for custody or the person's restore code (§12.1).
    pub fn to_secret_bytes(&self) -> SecretBytes {
        let mut bytes = Zeroizing::new(Vec::with_capacity(69));
        bytes.extend_from_slice(b"CVMK\x01");
        bytes.extend_from_slice(self.signing.as_bytes());
        bytes.extend_from_slice(self.sealing.as_bytes());
        SecretBytes(bytes)
    }

    /// Restore both pairs from custody or the person's restore code (§12.1).
    pub fn from_secret_bytes(mut bytes: &[u8]) -> Result<Self, MaterialError> {
        wire::prefix(&mut bytes, b"CVMK\x01")?;
        let signing = Zeroizing::new(wire::array(&mut bytes)?);
        let sealing = Zeroizing::new(wire::array(&mut bytes)?);
        wire::end(bytes)?;
        Ok(Self {
            signing: SigningKey::from_bytes(&signing),
            sealing: StaticSecret::from(*sealing),
        })
    }

    /// Open the store key sealed to this member at the supplied storage path.
    /// Panics if the storage path is empty.
    pub fn open_store_key(&self, path: &str, sealed: &[u8]) -> Result<StoreKey, CryptoError> {
        let plaintext = self.open_box(b"store", 48, path, sealed)?;
        let mut bytes = plaintext.as_slice();
        let key = StoreKey::decode(&mut bytes)?;
        wire::end(bytes)?;
        Ok(key)
    }

    /// Open the circle key sealed to this member at the supplied storage path.
    /// Panics if the storage path is empty.
    pub fn open_circle_key(&self, path: &str, sealed: &[u8]) -> Result<CircleKey, CryptoError> {
        let plaintext = self.open_box(b"circle", 64, path, sealed)?;
        let mut bytes = plaintext.as_slice();
        let key = CircleKey::decode(&mut bytes)?;
        wire::end(bytes)?;
        Ok(key)
    }

    fn open_box(
        &self,
        kind: &[u8],
        plaintext_length: usize,
        path: &str,
        sealed: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        let envelope = SealedKey::decode(sealed)?;
        if envelope.body.len() != plaintext_length + cipher::SEALED_OBJECT_CHUNK_OVERHEAD {
            return Err(CryptoError::Malformed);
        }
        let sender = envelope.ephemeral;
        let recipient = self.sealing_public_key();
        let shared = self.sealing.diffie_hellman(&PublicKey::from(sender));
        if !shared.was_contributory() {
            return Err(CryptoError::WeakSealingKey);
        }
        let context = box_context(kind, path, &sender, recipient.as_bytes());
        let key = derivation::derive(shared.as_bytes(), &context);
        Ok(Zeroizing::new(cipher::open_random(
            &key,
            &context,
            envelope.body,
        )?))
    }
}

impl fmt::Debug for MemberKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MemberKeys([REDACTED])")
    }
}

/// Seal a store key to a member anonymously, authenticating its storage path (§11).
/// Panics if the storage path is empty.
pub fn seal_store_key(
    key: &StoreKey,
    recipient: &SealingPublicKey,
    path: &str,
) -> Result<Vec<u8>, CryptoError> {
    let mut bytes = Zeroizing::new(Vec::with_capacity(48));
    key.encode_into(&mut bytes);
    seal_box(b"store", recipient, path, &bytes)
}

/// Seal a circle key to a member anonymously, authenticating its storage path (§14.3).
/// Panics if the storage path is empty.
pub fn seal_circle_key(
    key: &CircleKey,
    recipient: &SealingPublicKey,
    path: &str,
) -> Result<Vec<u8>, CryptoError> {
    let mut bytes = Zeroizing::new(Vec::with_capacity(64));
    key.encode_into(&mut bytes);
    seal_box(b"circle", recipient, path, &bytes)
}

fn object_message(path: &str, digest: &ObjectDigest) -> Vec<u8> {
    cipher::context(&[
        b"coven/object-signature/v1",
        cipher::storage_path(path),
        digest.as_bytes(),
    ])
}

fn box_context(kind: &[u8], path: &str, sender: &[u8; 32], recipient: &[u8; 32]) -> Vec<u8> {
    cipher::context(&[
        derivation::SEALED_BOX,
        kind,
        cipher::storage_path(path),
        sender,
        recipient,
    ])
}

fn seal_box(
    kind: &[u8],
    recipient: &SealingPublicKey,
    path: &str,
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let mut seed = Zeroizing::new([0; 32]);
    randomness::fill(seed.as_mut())?;
    let ephemeral = StaticSecret::from(*seed);
    let sender = PublicKey::from(&ephemeral).to_bytes();
    let shared = ephemeral.diffie_hellman(&PublicKey::from(recipient.0));
    if !shared.was_contributory() {
        return Err(CryptoError::WeakSealingKey);
    }
    let context = box_context(kind, path, &sender, &recipient.0);
    let key = derivation::derive(shared.as_bytes(), &context);
    let body = cipher::seal_random(&key, &context, plaintext)?;
    Ok(SealedKey {
        ephemeral: sender,
        body: &body,
    }
    .encode())
}

#[cfg(test)]
#[path = "member_tests.rs"]
mod tests;
