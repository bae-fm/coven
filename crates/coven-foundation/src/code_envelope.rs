//! Shared wire format for pasted coven codes: `prefix + base64url(json)`.
//!
//! Invite codes and restore codes are both a JSON payload wrapped the same
//! way — a recognizable prefix, then the payload base64url-encoded — so they
//! share one implementation of that mechanics. A join-request code carries no
//! prefix; it reuses the same functions with an empty one, since an empty
//! prefix always strips and never rejects.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Prefix on a pasted coven code, so a string from an unrelated format is
/// rejected immediately with a clear "missing prefix" error rather than
/// failing confusingly at base64 or JSON decode.
pub const PREFIX: &str = "coven:";

/// An envelope-level decode failure. Each caller maps this to its own
/// user-facing error type.
pub enum EnvelopeError {
    MissingPrefix,
    InvalidBase64,
    InvalidJson(String),
}

/// Encode `code` as `{prefix}{base64url(json)}`.
pub fn encode_code<T: Serialize>(prefix: &str, code: &T) -> String {
    let json = serde_json::to_vec(code).expect("code is always serializable");
    let b64 = URL_SAFE_NO_PAD.encode(&json);
    format!("{prefix}{b64}")
}

/// Decode `{prefix}{base64url(json)}` back into `T`. Trims surrounding
/// whitespace first, so a pasted code with stray leading/trailing newlines
/// still decodes.
pub fn decode_code<T: DeserializeOwned>(prefix: &str, s: &str) -> Result<T, EnvelopeError> {
    let trimmed = s.trim();
    let payload = trimmed
        .strip_prefix(prefix)
        .ok_or(EnvelopeError::MissingPrefix)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| EnvelopeError::InvalidBase64)?;
    serde_json::from_slice(&bytes).map_err(|e| EnvelopeError::InvalidJson(e.to_string()))
}

/// Decode fixed-length hex material carried inside a pasted code.
pub fn decode_fixed_hex(label: &str, value: &str, expected_len: usize) -> Result<Vec<u8>, String> {
    let bytes = hex::decode(value).map_err(|error| format!("{label} is not hex: {error}"))?;
    if bytes.len() != expected_len {
        return Err(format!(
            "{label} must be {expected_len} bytes, got {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}
