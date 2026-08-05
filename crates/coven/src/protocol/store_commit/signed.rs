use serde::{Deserialize, Serialize};

use super::{domain_json, ObjectHash, StoreProtocolError, STORE_PROTOCOL_VERSION};
use crate::keys;

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signed<T> {
    version: u32,
    body: T,
    signature: String,
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
    pub(crate) fn verify_by(&self, public_key: &str) -> Result<(), StoreProtocolError> {
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
        ObjectHash::digest(&domain_json(
            T::DOMAIN,
            &SignedFields {
                version: self.version,
                body: &self.body,
            },
        ))
    }

    pub(crate) fn body(&self) -> &T {
        &self.body
    }

    /// The body, mutable, leaving the signature over whatever it held before.
    /// A draft artifact is built against objects whose slots are only allocated
    /// later, so its body is edited into final form and then [`Self::resign`]ed;
    /// a test uses this to build the tampered forms a verifier has to reject.
    /// Every reader of the value between the two calls sees a signature that
    /// does not check out.
    pub(crate) fn body_mut(&mut self) -> &mut T {
        &mut self.body
    }

    /// Sign the body this value now holds, replacing any earlier signature.
    pub(crate) fn resign<A: keys::IdentityKeyAuthority + ?Sized>(&mut self, signer: &A) {
        self.signature = keys::sign_hex(signer, self.digest().as_bytes()).1;
    }

    /// Sign `body` with a device authority — a retained capability that signs
    /// on a device's behalf without exposing the key [`Self::sign`] takes.
    pub(crate) fn sign_by_device(body: T, signer: &dyn keys::DeviceSigningAuthority) -> Self {
        let mut value = Self {
            version: STORE_PROTOCOL_VERSION,
            body,
            signature: String::new(),
        };
        value.signature = hex::encode(signer.sign(value.digest().as_bytes()));
        value
    }

    /// Damage the signature so verification fails, for tests that assert a
    /// verifier refuses an artifact whose signature does not check out.
    #[cfg(test)]
    pub(crate) fn corrupt_signature_for_test(&mut self) {
        self.signature.push('0');
    }
}

impl<T> Signed<T> {
    /// An envelope carrying no signature, for tests that need an artifact's
    /// shape somewhere no verifier reads it.
    #[cfg(test)]
    pub(crate) fn unsigned_for_test(body: T) -> Self {
        Self {
            version: STORE_PROTOCOL_VERSION,
            body,
            signature: String::new(),
        }
    }
}

impl<T: Serialize> Signed<T> {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
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
