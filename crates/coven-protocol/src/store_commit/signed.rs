use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use super::{domain_json, ObjectHash, StoreProtocolError, STORE_PROTOCOL_VERSION};
use coven_keys::keys;

/// A value that travels signed. The body names the domain its signature is
/// bound to, so a signature over one artifact can never be replayed as another.
///
/// Everything the body holds is signed, structurally: [`Signed`] serializes the
/// whole body to produce the signed bytes. A field added to a body is covered
/// the moment it exists, with no separate list to keep in step.
pub trait SignedBody: Serialize {
    const DOMAIN: &'static [u8];
}

/// One signed artifact: the protocol version it was written under, the body,
/// and the signature over both.
///
/// The version lives here rather than inside each body because it says the same
/// thing about every artifact. It is inside the signed bytes, so it cannot be
/// edited without invalidating the signature.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signed<T> {
    version: u32,
    body: T,
    signature: String,
    /// The digest of the signed bytes, computed the first time one is asked
    /// for and held so that hashing, verifying, and re-signing the same
    /// artifact serialize its body once rather than once per call. It is
    /// derived state: it never crosses the wire, never enters equality, and
    /// [`Signed::body_mut`] — the only way the bytes it covers can change —
    /// drops it.
    #[serde(skip)]
    digest: OnceLock<ObjectHash>,
}

/// Two signed artifacts are the same artifact when they were written under the
/// same version, carry the same body, and bear the same signature. The cached
/// digest is a function of the first two, so it says nothing equality does not.
impl<T: PartialEq> PartialEq for Signed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.body == other.body
            && self.signature == other.signature
    }
}

impl<T: Eq> Eq for Signed<T> {}

/// Printed like the three fields that make up the artifact, so that two values
/// that compare equal also read the same regardless of whether either has been
/// hashed yet.
impl<T: std::fmt::Debug> std::fmt::Debug for Signed<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Signed")
            .field("version", &self.version)
            .field("body", &self.body)
            .field("signature", &self.signature)
            .finish()
    }
}

/// What the signature covers: the version and the body, never the signature.
#[derive(Serialize)]
struct SignedFields<'a, T> {
    version: u32,
    body: &'a T,
}

impl<T: SignedBody> Signed<T> {
    /// Sign `body` under this build's protocol version.
    pub(crate) fn sign<A: keys::IdentityKeyAuthority + ?Sized>(body: T, signer: &A) -> Self {
        let mut value = Self {
            version: STORE_PROTOCOL_VERSION,
            body,
            signature: String::new(),
            digest: OnceLock::new(),
        };
        value.resign(signer);
        value
    }

    /// Refuse an artifact written under a version this build does not read.
    /// [`Self::verify_by`] runs this first; a shape check that has no signer to
    /// verify against calls it on its own.
    pub(crate) fn require_version(&self) -> Result<(), StoreProtocolError> {
        super::require_version(self.version)
    }

    /// Check the signature against `public_key`, refusing a version this build
    /// does not read before spending a verification on it.
    pub fn verify_by(&self, public_key: &str) -> Result<(), StoreProtocolError> {
        self.require_version()?;
        if keys::verify_signature_hex(public_key, &self.signature, self.digest().as_bytes()) {
            Ok(())
        } else {
            Err(StoreProtocolError::InvalidSignature)
        }
    }

    /// The artifact's identity: the digest of its domain-separated signed bytes.
    pub(crate) fn hash(&self) -> ObjectHash {
        self.digest()
    }

    fn digest(&self) -> ObjectHash {
        *self.digest.get_or_init(|| {
            ObjectHash::digest(&domain_json(
                T::DOMAIN,
                &SignedFields {
                    version: self.version,
                    body: &self.body,
                },
            ))
        })
    }

    pub(crate) fn body(&self) -> &T {
        &self.body
    }

    /// The body, mutable, leaving the signature over whatever it held before.
    /// A draft artifact is built against objects whose slots are only allocated
    /// later, so its body is edited into final form and then [`Self::resign`]ed;
    /// a test uses this to build the tampered forms a verifier has to reject.
    /// Every reader of the value between the two calls sees a signature that
    /// does not check out. The cached digest goes with the old body: the next
    /// hash, verification, or re-signing is taken over the bytes as edited.
    pub fn body_mut(&mut self) -> &mut T {
        self.digest = OnceLock::new();
        &mut self.body
    }

    /// Sign the body this value now holds, replacing any earlier signature.
    /// The signature is not part of what the digest covers, so an artifact
    /// signed again is still identified by the same hash.
    pub fn resign<A: keys::IdentityKeyAuthority + ?Sized>(&mut self, signer: &A) {
        self.signature = keys::sign_hex(signer, self.digest().as_bytes()).1;
    }

    /// Sign `body` with a device authority — a retained capability that signs
    /// on a device's behalf without exposing the key [`Self::sign`] takes.
    pub(crate) fn sign_by_device(body: T, signer: &dyn keys::DeviceSigningAuthority) -> Self {
        let mut value = Self {
            version: STORE_PROTOCOL_VERSION,
            body,
            signature: String::new(),
            digest: OnceLock::new(),
        };
        value.signature = hex::encode(signer.sign(value.digest().as_bytes()));
        value
    }

    /// Damage the signature so verification fails, for tests that assert a
    /// verifier refuses an artifact whose signature does not check out.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn corrupt_signature_for_test(&mut self) {
        self.signature.push('0');
    }
}

impl<T> Signed<T> {
    /// An envelope carrying no signature, for tests that need an artifact's
    /// shape somewhere no verifier reads it.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn unsigned_for_test(body: T) -> Self {
        Self {
            version: STORE_PROTOCOL_VERSION,
            body,
            signature: String::new(),
            digest: OnceLock::new(),
        }
    }
}

impl<T: Serialize> Signed<T> {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a signed artifact serializes")
    }
}

/// Reading a signed artifact's fields reads its body. The envelope's own parts —
/// the version and the signature — are reached through its methods, so a body
/// field can never be shadowed by one of them.
impl<T> std::ops::Deref for Signed<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keys::UserKeypair;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Note {
        text: String,
    }

    impl SignedBody for Note {
        const DOMAIN: &'static [u8] = b"store-v1/test/note";
    }

    fn note(text: &str, signer: &UserKeypair) -> Signed<Note> {
        Signed::sign(
            Note {
                text: text.to_string(),
            },
            signer,
        )
    }

    #[test]
    fn an_artifact_hashes_to_the_same_value_every_time_it_is_asked() {
        let signer = UserKeypair::generate();
        let note = note("first", &signer);

        let hash = note.hash();

        assert_eq!(hash, note.hash());
        assert_eq!(hash, note.hash());
    }

    #[test]
    fn an_edited_body_hashes_as_the_body_it_now_holds() {
        let signer = UserKeypair::generate();
        let mut edited = note("first", &signer);
        let before = edited.hash();

        edited.body_mut().text = "second".to_string();

        assert_ne!(before, edited.hash());
        assert_eq!(edited.hash(), note("second", &signer).hash());
    }

    #[test]
    fn an_edited_body_fails_verification_until_it_is_signed_again() {
        let signer = UserKeypair::generate();
        let public_key = keys::public_key_hex(&signer);
        let mut edited = note("first", &signer);
        edited.verify_by(&public_key).unwrap();

        edited.body_mut().text = "second".to_string();

        assert!(matches!(
            edited.verify_by(&public_key),
            Err(StoreProtocolError::InvalidSignature)
        ));
        edited.resign(&signer);
        edited.verify_by(&public_key).unwrap();
    }

    #[test]
    fn signing_again_leaves_the_artifact_identified_by_the_same_hash() {
        let signer = UserKeypair::generate();
        let mut resigned = note("first", &signer);
        let before = resigned.hash();

        resigned.resign(&UserKeypair::generate());

        assert_eq!(before, resigned.hash());
    }

    #[test]
    fn a_round_trip_through_json_keeps_the_artifact_equal_verifiable_and_identified() {
        let signer = UserKeypair::generate();
        let public_key = keys::public_key_hex(&signer);
        let original = note("first", &signer);
        let hash = original.hash();

        let parsed: Signed<Note> = serde_json::from_slice(&original.to_bytes()).unwrap();

        assert_eq!(parsed, original);
        parsed.verify_by(&public_key).unwrap();
        assert_eq!(hash, parsed.hash());
    }
}
