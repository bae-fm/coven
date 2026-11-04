//! Cryptographic primitives for native S3 request authentication (§20.7).
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
    v4_signature(("GOOG4", "goog4_request"), secret, [date, region, "storage"], message)
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
    v4_signature(("AWS4", "aws4_request"), secret, [date, region, service], message)
}

/// The V4 scheme both services share: the signing key chains an HMAC from
/// the prefixed secret through each scope part and the scope's terminator,
/// then signs `message`. Only the prefix and terminator name the service.
fn v4_signature(
    (prefix, terminator): (&str, &str),
    secret: &str,
    scope: [&str; 3],
    message: &[u8],
) -> [u8; 32] {
    let secret = Zeroizing::new(format!("{prefix}{secret}"));
    let mut key = hmac(secret.as_bytes(), scope[0].as_bytes());
    for part in [scope[1], scope[2], terminator] {
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
