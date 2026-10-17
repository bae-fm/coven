//! A member's signing and sealing key pairs, kept together (§11.1).

use crate::{cipher, derivation, randomness, wire};
use crate::{CircleKey, CryptoError, MaterialError, ObjectDigest, SecretBytes, StoreKey};
use coven_foundation::id_source::MemberId;
use ed25519_dalek::{Signer, VerifyingKey};
use std::fmt;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

/// A member's validated Ed25519 public signing key (§11.1). It is bound to a
/// stable [`MemberId`] by admin entries and is never that identity.
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

    /// Verify a write's header signature without its part chunks (D9): the
    /// exact cleartext prefix, the complete sealed header chunk and the part
    /// roots. Snapshots carry these to authenticate another member's leaves.
    /// Panics if the storage path is empty.
    pub fn verify_write_header(
        &self,
        path: &str,
        prefix: &[u8],
        header: &[u8],
        roots: &[[u8; 32]],
        signature: &Signature,
    ) -> Result<(), CryptoError> {
        self.verify(&header_message(path, prefix, header, roots), signature)
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

    /// The public pair admin entries bind to a stable member id (§9).
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
    /// member.sign_object("10101010-1010-1010-1010-101010101010/seq/1", &ContentHasher::new().finish());
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

    /// Sign a write's cleartext prefix, sealed header chunk and part roots
    /// (D9), separately from the whole-object signature.
    /// Panics if the storage path is empty.
    pub fn sign_write_header(
        &self,
        path: &str,
        prefix: &[u8],
        header: &[u8],
        roots: &[[u8; 32]],
    ) -> Signature {
        self.sign(&header_message(path, prefix, header, roots))
    }

    /// Encode both private seeds for custody or the person's restore code (§12.1).
    pub fn to_secret_bytes(&self) -> SecretBytes {
        let mut bytes = Zeroizing::new(Vec::with_capacity(MEMBER_KEYS_LENGTH));
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

/// This device's member: the stable id and every private pair it holds
/// (E1, E11). Identity custody keeps it; the id never derives from the keys.
/// The active pair is the one the last installed binding made current. A
/// self-replacement keeps its candidate here before publishing it, and every
/// pair a kept replacement retired stays held, because copies name the exact
/// credential they open with (§11). Which pair may sign is replay's decision,
/// never this record's (§10).
#[derive(Clone)]
pub struct MemberIdentity {
    member: MemberId,
    keys: MemberKeys,
    retained: Vec<MemberKeys>,
    candidate: Option<MemberKeys>,
}

impl MemberIdentity {
    /// Bind a private pair to the stable id its admin entry names.
    pub fn new(member: MemberId, keys: MemberKeys) -> Self {
        Self {
            member,
            keys,
            retained: Vec::new(),
            candidate: None,
        }
    }

    /// Make both key pairs for an independently generated id (§9).
    pub fn generate(member: MemberId) -> Result<Self, CryptoError> {
        Ok(Self::new(member, MemberKeys::generate()?))
    }

    /// The stable member id.
    pub fn member(&self) -> MemberId {
        self.member
    }

    /// The active private pair: the one this device signs with and a
    /// restore code exports.
    pub fn keys(&self) -> &MemberKeys {
        &self.keys
    }

    /// A self-replacement's fresh pair, kept before its binding is published.
    pub fn candidate(&self) -> Option<&MemberKeys> {
        self.candidate.as_ref()
    }

    /// Every held pair: the active one, retired ones, then the candidate.
    fn held(&self) -> impl Iterator<Item = &MemberKeys> {
        std::iter::once(&self.keys)
            .chain(&self.retained)
            .chain(&self.candidate)
    }

    /// The held private pair whose sealing key is `sealing`. A copy names the
    /// exact credential it is sealed to, and opens only with that one, never
    /// with whichever pair is latest (§11).
    pub fn credential(&self, sealing: &SealingKey) -> Option<&MemberKeys> {
        self.held()
            .find(|keys| keys.public_keys().sealing == *sealing)
    }

    /// Whether some held pair is exactly `keys`.
    pub fn holds(&self, keys: &MemberPublicKeys) -> bool {
        self.held().any(|held| held.public_keys() == *keys)
    }

    /// This identity keeping `candidate` for a self-replacement. A candidate
    /// already held stays held as a retired credential: its copies may exist.
    pub fn with_candidate(&self, candidate: MemberKeys) -> Self {
        let mut next = self.clone();
        next.retained.extend(next.candidate.replace(candidate));
        next
    }

    /// This identity with the held pair `current` active, as replay's current
    /// binding selects it (§10). The previously active pair and any other
    /// candidate stay held as retired credentials. `None` when no held pair
    /// is `current`.
    pub fn activate(&self, current: &MemberPublicKeys) -> Option<Self> {
        if self.keys.public_keys() == *current && self.candidate.is_none() {
            return Some(self.clone());
        }
        let mut pairs: Vec<MemberKeys> = self.held().cloned().collect();
        let index = pairs
            .iter()
            .position(|keys| keys.public_keys() == *current)?;
        let keys = pairs.remove(index);
        let mut retained = Vec::new();
        for pair in pairs {
            if !retained
                .iter()
                .any(|held: &MemberKeys| held.public_keys() == pair.public_keys())
            {
                retained.push(pair);
            }
        }
        Some(Self {
            member: self.member,
            keys,
            retained,
            candidate: None,
        })
    }

    /// This identity with its candidate retired: a replacement that never
    /// became current leaves its copies, so its pair stays held (§11).
    pub fn retire_candidate(&self) -> Self {
        let mut next = self.clone();
        next.retained.extend(next.candidate.take());
        next
    }

    /// This identity without its candidate: a self-replacement cancelled
    /// before reserving anything never sent a copy to it.
    pub fn without_candidate(&self) -> Self {
        let mut next = self.clone();
        next.candidate = None;
        next
    }

    /// Encode the id and every held private pair for identity custody: the
    /// active pair, the retired ones, then the candidate if any. This custody
    /// encoding is not a restore code (D13).
    pub fn to_secret_bytes(&self) -> SecretBytes {
        // Allocate once so growth cannot leave an unerased copy of the seeds.
        let pairs = 1 + self.retained.len() + usize::from(self.candidate.is_some());
        let mut bytes = Zeroizing::new(Vec::with_capacity(30 + pairs * MEMBER_KEYS_LENGTH));
        bytes.extend_from_slice(b"CVMI\x01");
        bytes.extend_from_slice(self.member.0.as_bytes());
        bytes.extend_from_slice(self.keys.to_secret_bytes().as_bytes());
        bytes.extend_from_slice(&(self.retained.len() as u64).to_le_bytes());
        for keys in &self.retained {
            bytes.extend_from_slice(keys.to_secret_bytes().as_bytes());
        }
        match &self.candidate {
            Some(keys) => {
                bytes.push(1);
                bytes.extend_from_slice(keys.to_secret_bytes().as_bytes());
            }
            None => bytes.push(0),
        }
        SecretBytes(bytes)
    }

    /// Restore the id and every held pair from identity custody.
    pub fn from_secret_bytes(mut bytes: &[u8]) -> Result<Self, MaterialError> {
        fn pair(bytes: &mut &[u8]) -> Result<MemberKeys, MaterialError> {
            MemberKeys::from_secret_bytes(wire::take(bytes, MEMBER_KEYS_LENGTH)?)
        }
        wire::prefix(&mut bytes, b"CVMI\x01")?;
        let member = MemberId(uuid::Uuid::from_bytes(wire::array(&mut bytes)?));
        let keys = pair(&mut bytes)?;
        let count = wire::number(&mut bytes)?;
        if count > (bytes.len() / MEMBER_KEYS_LENGTH) as u64 {
            return Err(MaterialError::Encoding);
        }
        let retained = (0..count)
            .map(|_| pair(&mut bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let candidate = match wire::array(&mut bytes)? {
            [0] => None,
            [1] => Some(pair(&mut bytes)?),
            _ => return Err(MaterialError::Encoding),
        };
        wire::end(bytes)?;
        let identity = Self {
            member,
            keys,
            retained,
            candidate,
        };
        // Each credential is held once.
        let mut public: Vec<_> = identity.held().map(MemberKeys::public_keys).collect();
        public.sort();
        public.dedup();
        if public.len() != identity.held().count() {
            return Err(MaterialError::Encoding);
        }
        Ok(identity)
    }
}

/// The length of a private-pair blob, `CVMK 01` and both 32-byte secrets.
const MEMBER_KEYS_LENGTH: usize = 69;

impl fmt::Debug for MemberIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemberIdentity")
            .field("member", &self.member)
            .field("retained", &self.retained.len())
            .field("candidate", &self.candidate.is_some())
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

fn header_message(path: &str, prefix: &[u8], header: &[u8], roots: &[[u8; 32]]) -> Vec<u8> {
    let roots: Vec<u8> = roots.iter().flatten().copied().collect();
    cipher::context(&[
        b"coven/write-header-signature/v1",
        cipher::storage_path(path),
        prefix,
        header,
        &roots,
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
