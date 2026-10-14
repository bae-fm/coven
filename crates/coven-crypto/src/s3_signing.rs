//! Cryptographic primitives for native S3 request authentication (§4).
//! Request canonicalization belongs to the storage adapter.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

/// SHA-256 of a native request body or canonical request, before V4 signing.
pub fn s3_request_hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Google V4 HMAC signature using the account's interoperability secret.
/// Date and region are the exact credential-scope components in the request.
/// Intermediate signing keys and the prefixed secret are erased on drop.
pub fn gcs_request_signature(secret: &str, date: &str, region: &str, message: &[u8]) -> [u8; 32] {
    let secret = Zeroizing::new(format!("GOOG4{secret}"));
    let mut key = hmac(secret.as_bytes(), date.as_bytes());
    for part in [region, "storage", "goog4_request"] {
        key = hmac(key.as_ref(), part.as_bytes());
    }
    *hmac(key.as_ref(), message)
}

/// AWS Signature Version 4 over a request's string to sign. The signing key
/// derives from the secret through the date, region and service scope;
/// intermediate keys and the prefixed secret are erased on drop.
pub fn aws_request_signature(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    message: &[u8],
) -> [u8; 32] {
    let secret = Zeroizing::new(format!("AWS4{secret}"));
    let mut key = hmac(secret.as_bytes(), date.as_bytes());
    for part in [region, service, "aws4_request"] {
        key = hmac(key.as_ref(), part.as_bytes());
    }
    *hmac(key.as_ref(), message)
}

fn hmac(key: &[u8], bytes: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(bytes);
    Zeroizing::new(mac.finalize().into_bytes().into())
}

#[cfg(test)]
#[path = "s3_signing_tests.rs"]
mod tests;
