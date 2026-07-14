//! Flat key encoding shared by Google Drive and OneDrive.
//!
//! Both store every object flat in a single folder, so a `/`-bearing CloudHome
//! key is hex-encoded into a slash-free filename and back. The encoding was
//! byte-identical in both backends; this is the one copy.

use std::fmt;
use tracing::warn;

#[derive(Debug)]
pub(super) enum DecodeKeyError {
    Hex(hex::FromHexError),
    Utf8(std::string::FromUtf8Error),
}

impl fmt::Display for DecodeKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hex(e) => write!(f, "invalid hex: {e}"),
            Self::Utf8(e) => write!(f, "invalid utf-8: {e}"),
        }
    }
}

impl std::error::Error for DecodeKeyError {}

/// `objects/dev1/42.enc` -> `6f626a656374732f646576312f34322e656e63`.
pub(super) fn encode_key(key: &str) -> String {
    hex::encode(key.as_bytes())
}

/// `6f626a656374732f646576312f34322e656e63` -> `objects/dev1/42.enc`.
pub(super) fn decode_key(filename: &str) -> Result<String, DecodeKeyError> {
    let bytes = hex::decode(filename).map_err(DecodeKeyError::Hex)?;
    String::from_utf8(bytes).map_err(DecodeKeyError::Utf8)
}

pub(super) fn decode_listed_key(provider: &str, filename: &str) -> Option<String> {
    match decode_key(filename) {
        Ok(decoded) => Some(decoded),
        Err(e) => {
            warn!("skipping {provider} object with malformed flat name {filename}: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        for key in [
            "objects/snapshot-image.db.enc",
            "objects/current.json.enc",
            "objects/device-abc/1.enc",
            "objects/device-abc.json.enc",
            "images/cover.jpg",
            "images/cover_art.jpg",
            "images/a__b.jpg",
            "images/a_/b.jpg",
            "images/x/_y.jpg",
        ] {
            assert_eq!(
                decode_key(&encode_key(key)).expect("decode encoded key"),
                key
            );
        }
    }

    #[test]
    fn encode_replaces_slashes() {
        assert_eq!(
            encode_key("objects/dev1/42.enc"),
            "6f626a656374732f646576312f34322e656e63",
        );
    }

    #[test]
    fn slash_encoding_does_not_collide_with_literal_underscores() {
        assert_ne!(encode_key("a/b"), encode_key("a__b"));
    }

    #[test]
    fn decode_rejects_malformed_flat_names() {
        assert!(decode_key("a___b").is_err());
        assert!(decode_key("_xx").is_err());
        assert!(decode_key("_2").is_err());
        assert!(decode_key("ff").is_err());
    }
}
