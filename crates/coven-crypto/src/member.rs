//! A member's signing and sealing key pairs, kept together (§11.1).

use crate::{cipher, derivation, randomness, wire};
use crate::{CircleKey, CryptoError, MaterialError, ObjectDigest, SecretBytes, StoreKey};
use coven_foundation::id_source::MemberId;
use ed25519_dalek::{Signer, VerifyingKey};
use std::fmt;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

/// A member's validated Ed25519 public signing key (§11.1). It is bound to a
/// stable [`MemberId`] by the store log and is never that identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SigningKey([u8; 32]);

impl SigningKey {
    /// Validate a public signing key, refusing invalid and weak Ed25519 points.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, CryptoError> {
        let key = VerifyingKey::from_bytes(&bytes).map_err(|_| CryptoError::InvalidSigningKey)?;
        if key.is_weak() {
            return Err(CryptoError::InvalidSigningKey);
        }
        Ok(Self(bytes))
    }

    /// The validated Ed25519 public key's 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Verify a detached signature against this public key (§10).
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), CryptoError> {
        let key = VerifyingKey::from_bytes(&self.0)
            .expect("SigningKey contains a validated Ed25519 public key");
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

    /// Verify a snapshot prefix independently of its encrypted data (D9).
    /// `prefix` is exactly kind, version and routing fields, without signatures.
    /// Panics if the storage path is empty.
    pub fn verify_prefix(
        &self,
        path: &str,
        prefix: &[u8],
        signature: &Signature,
    ) -> Result<(), CryptoError> {
        self.verify(&prefix_message(path, prefix), signature)
    }
}

/// The public half of a member's X25519 pair, used for anonymous sealed keys.
/// A copy names the exact sealing key it is sealed to (D9).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SealingKey([u8; 32]);

impl SealingKey {
    /// Read a public sealing key. Low-order keys are refused when sealing.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// The public bytes carried with the member's join request (§12.2).
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A member's public signing and sealing keys, independent of their stable
/// [`MemberId`] (§9, D2). Creation, addition and replacement bind a pair to an id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct MemberPublicKeys {
    /// The Ed25519 key that verifies the member's signatures.
    pub signing: SigningKey,
    /// The X25519 key to which store and circle keys are sealed.
    pub sealing: SealingKey,
}

/// A detached Ed25519 signature on bytes authored by a member (§10).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Signature([u8; 64]);

impl Signature {
    /// Read a detached signature; authenticity is checked by `SigningKey::verify`.
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
    /// The signature bytes written beside the signed message.
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

/// A member's Ed25519 signing pair and independent X25519 sealing pair (§11.1).
/// Both secrets erase on drop. Clone is needed by in-memory custody to keep
/// its keys while returning an independently owned unlocked pair.
#[derive(Clone)]
pub struct MemberKeys {
    signing: ed25519_dalek::SigningKey,
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
            signing: ed25519_dalek::SigningKey::from_bytes(&signing),
            sealing: StaticSecret::from(*sealing),
        })
    }

    /// The public pair a store log binds to a stable member id (§9).
    pub fn public_keys(&self) -> MemberPublicKeys {
        MemberPublicKeys {
            signing: SigningKey(self.signing.verifying_key().to_bytes()),
            sealing: SealingKey(PublicKey::from(&self.sealing).to_bytes()),
        }
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
    /// member.sign_object("10101010-1010-1010-1010-101010101010/devices/1/1", &ContentHasher::new().finish());
    /// ```
    pub fn sign_object(&self, path: &str, digest: &ObjectDigest) -> Signature {
        self.sign(&object_message(path, digest))
    }

    /// Sign a snapshot's exact kind, version and routing fields (D9).
    /// The prefix domain and path are bound separately from whole-object signing.
    /// Panics if the storage path is empty.
    pub fn sign_prefix(&self, path: &str, prefix: &[u8]) -> Signature {
        self.sign(&prefix_message(path, prefix))
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
            signing: ed25519_dalek::SigningKey::from_bytes(&signing),
            sealing: StaticSecret::from(*sealing),
        })
    }

    /// Open a store-key box, authenticating the exact path and opaque clear prefix.
    /// Format checks the recipient and opened identity; the caller checks the
    /// introduction commitment before accepting the candidate key.
    /// Panics if the storage path is empty.
    pub fn open_store_key(
        &self,
        path: &str,
        prefix: &[u8],
        sealed: &[u8],
    ) -> Result<StoreKey, CryptoError> {
        let plaintext = self.open_box(b"store", 48, path, prefix, sealed)?;
        let mut bytes = plaintext.as_slice();
        let key = StoreKey::decode(&mut bytes)?;
        wire::end(bytes)?;
        Ok(key)
    }

    /// Open a circle-key box, authenticating the exact path and opaque clear prefix.
    /// Format checks the recipient and opened identity; the caller checks the
    /// introduction commitment before accepting the candidate key.
    /// Panics if the storage path is empty.
    pub fn open_circle_key(
        &self,
        path: &str,
        prefix: &[u8],
        sealed: &[u8],
    ) -> Result<CircleKey, CryptoError> {
        let plaintext = self.open_box(b"circle", 64, path, prefix, sealed)?;
        let mut bytes = plaintext.as_slice();
        let key = CircleKey::decode(&mut bytes)?;
        wire::end(bytes)?;
        Ok(key)
    }

    /// Open a member's delivered S3 key box, authenticating the exact path and
    /// clear prefix (§13). Its plaintext is at most `MAX_ACCESS_KEY_BOX`
    /// bytes; format decodes it. Panics if the storage path is empty.
    pub fn open_access_key(
        &self,
        path: &str,
        prefix: &[u8],
        sealed: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        let length = sealed
            .len()
            .checked_sub(32 + cipher::SEALED_OBJECT_CHUNK_OVERHEAD)
            .filter(|length| *length <= MAX_ACCESS_KEY_BOX)
            .ok_or(CryptoError::Malformed)?;
        self.open_box(b"s3-key", length, path, prefix, sealed)
    }

    fn open_box(
        &self,
        kind: &[u8],
        plaintext_length: usize,
        path: &str,
        prefix: &[u8],
        sealed: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        if sealed.len() != 32 + plaintext_length + cipher::SEALED_OBJECT_CHUNK_OVERHEAD {
            return Err(CryptoError::Malformed);
        }
        let sender: [u8; 32] = sealed[..32].try_into().expect("checked box length");
        let recipient = self.public_keys().sealing;
        let shared = self.sealing.diffie_hellman(&PublicKey::from(sender));
        if !shared.was_contributory() {
            return Err(CryptoError::WeakSealingKey);
        }
        let context = box_context(kind, path, prefix, &sender, recipient.as_bytes());
        let key = derivation::derive(shared.as_bytes(), &context);
        Ok(Zeroizing::new(cipher::open_random(
            &key,
            &context,
            &sealed[32..],
        )?))
    }
}

impl fmt::Debug for MemberKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MemberKeys([REDACTED])")
    }
}

/// This device's member: the stable id and the private pair it signs and
/// opens copies with (E1, E11). Identity custody keeps it; the id never
/// derives from the keys.
#[derive(Clone)]
pub struct MemberIdentity {
    member: MemberId,
    keys: MemberKeys,
}

impl MemberIdentity {
    /// Bind a private pair to the stable id the store log names for it.
    pub fn new(member: MemberId, keys: MemberKeys) -> Self {
        Self { member, keys }
    }

    /// Make both key pairs for an independently generated id (§9).
    pub fn generate(member: MemberId) -> Result<Self, CryptoError> {
        Ok(Self::new(member, MemberKeys::generate()?))
    }

    /// The stable member id.
    pub fn member(&self) -> MemberId {
        self.member
    }

    /// The private pair this device signs and opens copies with.
    pub fn keys(&self) -> &MemberKeys {
        &self.keys
    }

    /// The held private pair whose sealing key is `sealing`. A copy names the
    /// exact credential it is sealed to, and opens only with that one, never
    /// with whichever pair is latest (§11).
    pub fn credential(&self, sealing: &SealingKey) -> Option<&MemberKeys> {
        (self.keys.public_keys().sealing == *sealing).then_some(&self.keys)
    }

    /// Encode the id and private pair for identity custody. This custody
    /// encoding is not a restore code (D13).
    pub fn to_secret_bytes(&self) -> SecretBytes {
        let keys = self.keys.to_secret_bytes();
        let mut bytes = Zeroizing::new(Vec::with_capacity(21 + keys.as_bytes().len()));
        bytes.extend_from_slice(b"CVMI\x01");
        bytes.extend_from_slice(self.member.0.as_bytes());
        bytes.extend_from_slice(keys.as_bytes());
        SecretBytes(bytes)
    }

    /// Restore the id and pair from identity custody.
    pub fn from_secret_bytes(mut bytes: &[u8]) -> Result<Self, MaterialError> {
        wire::prefix(&mut bytes, b"CVMI\x01")?;
        let member = MemberId(uuid::Uuid::from_bytes(wire::array(&mut bytes)?));
        Ok(Self::new(member, MemberKeys::from_secret_bytes(bytes)?))
    }
}

impl fmt::Debug for MemberIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemberIdentity")
            .field("member", &self.member)
            .finish_non_exhaustive()
    }
}

/// Seal a store key anonymously, authenticating its path and exact clear prefix (D11).
/// Returns ephemeral key, nonce, ciphertext and tag; format supplies the prefix.
/// Panics if the storage path is empty.
pub fn seal_store_key(
    key: &StoreKey,
    recipient: &SealingKey,
    path: &str,
    prefix: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let mut bytes = Zeroizing::new(Vec::with_capacity(48));
    key.encode_into(&mut bytes);
    seal_box(b"store", recipient, path, prefix, &bytes)
}

/// Seal a circle key anonymously, authenticating its path and exact clear prefix (D11).
/// Returns ephemeral key, nonce, ciphertext and tag; format supplies the prefix.
/// Panics if the storage path is empty.
pub fn seal_circle_key(
    key: &CircleKey,
    recipient: &SealingKey,
    path: &str,
    prefix: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let mut bytes = Zeroizing::new(Vec::with_capacity(64));
    key.encode_into(&mut bytes);
    seal_box(b"circle", recipient, path, prefix, &bytes)
}

/// The largest delivered S3 key plaintext: its id, secret and principal (§13).
pub const MAX_ACCESS_KEY_BOX: usize = 8 * 1024;

/// Seal a member's new S3 key anonymously to their sealing key, binding the
/// path and exact clear prefix (§13). Format encodes the plaintext.
/// Panics if the storage path is empty.
pub fn seal_access_key(
    plaintext: &[u8],
    recipient: &SealingKey,
    path: &str,
    prefix: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if plaintext.len() > MAX_ACCESS_KEY_BOX {
        return Err(CryptoError::Malformed);
    }
    seal_box(b"s3-key", recipient, path, prefix, plaintext)
}

fn object_message(path: &str, digest: &ObjectDigest) -> Vec<u8> {
    cipher::context(&[
        b"coven/object-signature/v1",
        cipher::storage_path(path),
        digest.as_bytes(),
    ])
}

fn prefix_message(path: &str, prefix: &[u8]) -> Vec<u8> {
    cipher::context(&[
        b"coven/prefix-signature/v1",
        cipher::storage_path(path),
        prefix,
    ])
}

fn box_context(
    kind: &[u8],
    path: &str,
    prefix: &[u8],
    sender: &[u8; 32],
    recipient: &[u8; 32],
) -> Vec<u8> {
    cipher::context(&[
        derivation::SEALED_BOX,
        kind,
        cipher::storage_path(path),
        prefix,
        sender,
        recipient,
    ])
}

fn seal_box(
    kind: &[u8],
    recipient: &SealingKey,
    path: &str,
    prefix: &[u8],
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
    let context = box_context(kind, path, prefix, &sender, &recipient.0);
    let key = derivation::derive(shared.as_bytes(), &context);
    let body = cipher::seal_random(&key, &context, plaintext)?;
    let mut bytes = Vec::with_capacity(32 + body.len());
    bytes.extend_from_slice(&sender);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

#[cfg(test)]
#[path = "member_tests.rs"]
mod tests;
